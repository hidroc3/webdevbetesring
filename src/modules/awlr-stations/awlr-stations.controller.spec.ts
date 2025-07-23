import { Test, TestingModule } from '@nestjs/testing';
import { AwlrStationsController } from './awlr-stations.controller';

describe('AwlrStationsController', () => {
  let controller: AwlrStationsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AwlrStationsController],
    }).compile();

    controller = module.get<AwlrStationsController>(AwlrStationsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
