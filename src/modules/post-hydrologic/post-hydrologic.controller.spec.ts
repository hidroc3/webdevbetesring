import { Test, TestingModule } from '@nestjs/testing';
import { PostHydrologicController } from './post-hydrologic.controller';

describe('PostHydrologicController', () => {
  let controller: PostHydrologicController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [PostHydrologicController],
    }).compile();

    controller = module.get<PostHydrologicController>(PostHydrologicController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
